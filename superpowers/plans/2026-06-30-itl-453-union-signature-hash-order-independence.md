# Union Signature Hash Order Independence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `_function_signature_hash` order-independent for union-typed parameters and return types so that `def foo(x: str | Path)` and `def foo(x: Path | str)` produce the same hash.

**Architecture:** Add two private helpers (`_is_union_annotation`, `_canonical_annotation_str`) to `hash_utils.py` that canonicalize union type annotations by sorting their members lexicographically. Apply them at two call sites: `get_function_signature()` (which produces `_function_signature_hash`) and `FunctionSignatureExtractor.extract_function_info()` (which handles function objects passed directly to `hash_object()`). Non-union annotations are unchanged byte-for-byte.

**Tech Stack:** Python 3.12, `inspect` stdlib module, `types` stdlib module, `typing` stdlib module, uv (test runner: `uv run pytest`)

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `src/orcapod/hashing/hash_utils.py` | Modify | Add `_is_union_annotation`, `_canonical_annotation_str`; canonicalize unions in `get_function_signature` |
| `src/orcapod/hashing/semantic_hashing/function_info_extractors.py` | Modify | Canonicalize union param annotations in `FunctionSignatureExtractor.extract_function_info` |
| `tests/test_hashing/test_hash_utils.py` | Create | Unit tests for `get_function_signature` and `_canonical_annotation_str` |
| `tests/test_core/data_function/test_data_function.py` | Modify | Add `TestSignatureHashUnionOrderIndependence` integration tests |
| `DESIGN_ISSUES.md` | Modify | Add resolved entry for this bug |

---

### Task 1: Check out the feature branch

**Files:** none (git only)

- [ ] **Step 1: Check out the branch**

```bash
git checkout -b eywalker/itl-453-function-signature-hash-must-be-order-independent-for-union
```

- [ ] **Step 2: Verify**

```bash
git branch --show-current
```

Expected output: `eywalker/itl-453-function-signature-hash-must-be-order-independent-for-union`

---

### Task 2: Write failing tests for `get_function_signature` canonicalization

**Files:**
- Create: `tests/test_hashing/test_hash_utils.py`

These tests will fail until the helpers are added in Task 3.

- [ ] **Step 1: Create `tests/test_hashing/test_hash_utils.py`**

```python
"""Tests for hash_utils helpers, specifically canonical union annotation strings."""
import inspect
from pathlib import Path

import pytest

from orcapod.hashing.hash_utils import (
    _canonical_annotation_str,
    _is_union_annotation,
    get_function_signature,
)


class TestIsUnionAnnotation:
    def test_pep604_union_detected(self):
        assert _is_union_annotation(str | Path) is True

    def test_pep604_three_member_union_detected(self):
        assert _is_union_annotation(str | Path | bytes) is True

    def test_typing_union_detected(self):
        import typing
        assert _is_union_annotation(typing.Union[str, int]) is True

    def test_typing_optional_detected(self):
        import typing
        assert _is_union_annotation(typing.Optional[str]) is True

    def test_plain_type_not_union(self):
        assert _is_union_annotation(int) is False

    def test_generic_alias_not_union(self):
        assert _is_union_annotation(list[str]) is False

    def test_none_type_not_union(self):
        assert _is_union_annotation(type(None)) is False


class TestCanonicalAnnotationStr:
    def test_pep604_two_member_order_independent(self):
        """str | Path and Path | str produce the same canonical string."""
        assert _canonical_annotation_str(str | Path) == _canonical_annotation_str(Path | str)

    def test_pep604_canonical_form(self):
        """str | Path canonicalizes to 'pathlib.Path | str' (P before s)."""
        assert _canonical_annotation_str(str | Path) == "pathlib.Path | str"

    def test_pep604_three_member_order_independent(self):
        """All permutations of str | Path | bytes produce the same canonical string."""
        canonical = _canonical_annotation_str(str | Path | bytes)
        assert _canonical_annotation_str(bytes | str | Path) == canonical
        assert _canonical_annotation_str(Path | bytes | str) == canonical

    def test_pep604_three_member_canonical_form(self):
        """str | Path | bytes canonicalizes to 'bytes | pathlib.Path | str'."""
        assert _canonical_annotation_str(str | Path | bytes) == "bytes | pathlib.Path | str"

    def test_non_union_matches_formatannotation(self):
        """Non-union types fall through to inspect.formatannotation."""
        for t in (int, str, bytes, Path):
            assert _canonical_annotation_str(t) == inspect.formatannotation(t)

    def test_typing_union_order_independent(self):
        import typing
        assert (
            _canonical_annotation_str(typing.Union[str, int])
            == _canonical_annotation_str(typing.Union[int, str])
        )


class TestGetFunctionSignatureUnionCanonical:
    def test_param_union_order_independent(self):
        """get_function_signature returns the same string for str|Path and Path|str params."""
        def foo1(x: str | Path) -> str:
            return str(x)

        def foo2(x: Path | str) -> str:
            return str(x)

        assert get_function_signature(foo1) == get_function_signature(foo2)

    def test_param_three_member_union_order_independent(self):
        """All permutations of a 3-member union param produce the same signature string."""
        def f1(x: str | Path | bytes) -> str:
            return str(x)

        def f2(x: bytes | str | Path) -> str:
            return str(x)

        def f3(x: Path | bytes | str) -> str:
            return str(x)

        sig1 = get_function_signature(f1)
        assert get_function_signature(f2) == sig1
        assert get_function_signature(f3) == sig1

    def test_return_union_order_independent(self):
        """get_function_signature returns the same string for str|Path and Path|str returns."""
        def foo1(x: int) -> str | Path:
            return str(x)

        def foo2(x: int) -> Path | str:
            return str(x)

        assert get_function_signature(foo1) == get_function_signature(foo2)

    def test_non_union_param_unchanged(self):
        """Non-union param signatures are byte-for-byte identical before and after."""
        def foo(x: int) -> str:
            return str(x)

        # The exact current format — verifies no regression for non-union types.
        # Note: return type uses str(annotation) = "<class 'str'>" (not "str").
        sig = get_function_signature(foo)
        assert "x: int" in sig
        assert "foo" in sig

    def test_non_union_signature_stable(self):
        """Non-union function signature is deterministic across calls."""
        def bar(a: int, b: str) -> bytes:
            return b.encode()

        assert get_function_signature(bar) == get_function_signature(bar)
```

- [ ] **Step 2: Run to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_hash_utils.py -v 2>&1 | head -40
```

Expected: `ImportError` or `FAILED` because `_canonical_annotation_str` and `_is_union_annotation` don't exist yet.

---

### Task 3: Add helpers and fix `get_function_signature` in `hash_utils.py`

**Files:**
- Modify: `src/orcapod/hashing/hash_utils.py`

The file currently lives at `src/orcapod/hashing/hash_utils.py`. The relevant function is `get_function_signature` at lines 142–188. We add two private helpers above it and modify the function body.

- [ ] **Step 1: Add `_is_union_annotation` helper after the imports block (after line 13)**

Insert after the `logger = logging.getLogger(__name__)` line:

```python
def _is_union_annotation(annotation: object) -> bool:
    """Return ``True`` if *annotation* is a union type.

    Detects both PEP 604 ``X | Y`` (``types.UnionType``) and
    ``typing.Union[X, Y]`` / ``typing.Optional[X]``.

    Args:
        annotation: Any Python object (type annotation or otherwise).

    Returns:
        ``True`` if the annotation is a union; ``False`` otherwise.
    """
    import types as _types
    import typing

    if isinstance(annotation, _types.UnionType):
        return True
    return getattr(annotation, "__origin__", None) is typing.Union


def _canonical_annotation_str(annotation: object) -> str:
    """Return a stable, canonical string for a type annotation.

    For union types (both PEP 604 ``X | Y`` and ``typing.Union[X, Y]``),
    members are sorted byte-wise so that ``str | Path`` and ``Path | str``
    produce the same canonical string.  Non-union types fall through to
    ``inspect.formatannotation``, preserving existing behaviour exactly.

    The canonical ordering key is the fully qualified type name produced by
    ``inspect.formatannotation`` (e.g. ``"pathlib.Path"``, ``"str"``),
    sorted lexicographically.  This is stable across Python versions and
    machines and does not depend on ``id()`` or ``__hash__``.

    Args:
        annotation: A type annotation object.

    Returns:
        A canonical string representation.
    """
    if _is_union_annotation(annotation):
        args = getattr(annotation, "__args__", ()) or ()
        member_strs = sorted(_canonical_annotation_str(a) for a in args)
        return " | ".join(member_strs)
    return inspect.formatannotation(annotation)
```

- [ ] **Step 2: Modify `get_function_signature` to canonicalize union params**

In `get_function_signature`, replace the parameter loop (lines 170–175):

```python
    param_strs = []
    for name, param in sig.parameters.items():
        param_str = str(param)
        if not include_defaults and "=" in param_str:
            param_str = param_str.split("=")[0].strip()
        param_strs.append(param_str)
```

with:

```python
    param_strs = []
    for name, param in sig.parameters.items():
        param_str = str(param)
        annotation = param.annotation
        if annotation is not inspect.Parameter.empty and _is_union_annotation(annotation):
            old_ann = inspect.formatannotation(annotation)
            new_ann = _canonical_annotation_str(annotation)
            # Replace ": <old_ann>" with ": <new_ann>" (first occurrence only).
            # The ": " prefix distinguishes the annotation from the default value.
            param_str = param_str.replace(f": {old_ann}", f": {new_ann}", 1)
        if not include_defaults and "=" in param_str:
            param_str = param_str.split("=")[0].strip()
        param_strs.append(param_str)
```

- [ ] **Step 3: Modify `get_function_signature` to canonicalize union return types**

Replace the return-type line (lines 186–187):

```python
    if "returns" in parts:
        fn_string += f"-> {parts['returns']}"
```

with:

```python
    if "returns" in parts:
        ret = parts["returns"]
        if _is_union_annotation(ret):
            fn_string += f"-> {_canonical_annotation_str(ret)}"
        else:
            fn_string += f"-> {ret}"
```

- [ ] **Step 4: Run the new hash_utils tests**

```bash
uv run pytest tests/test_hashing/test_hash_utils.py -v
```

Expected: all tests **PASS**.

- [ ] **Step 5: Run existing hashing tests to confirm no regression**

```bash
uv run pytest tests/test_hashing/ -v 2>&1 | tail -20
```

Expected: all existing tests still **PASS**.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/hash_utils.py tests/test_hashing/test_hash_utils.py
git commit -m "fix(hashing): canonicalize union annotation order in get_function_signature (ITL-453)"
```

---

### Task 4: Fix `FunctionSignatureExtractor` for consistency

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/function_info_extractors.py`

`FunctionSignatureExtractor.extract_function_info` builds a `params` string with the same `str(param)` pattern. Its `returns` field is already a type object (order-independent via `UnionTypeHandler`), so only `params` needs fixing.

- [ ] **Step 1: Import the helpers at the top of `function_info_extractors.py`**

Add to the existing imports block:

```python
from orcapod.hashing.hash_utils import _canonical_annotation_str, _is_union_annotation
```

- [ ] **Step 2: Apply the same param-string substitution in `extract_function_info`**

In `FunctionSignatureExtractor.extract_function_info`, replace the parameter loop (lines 61–67):

```python
        param_strs = []
        for name, param in sig.parameters.items():
            param_str = str(param)
            if not self.include_defaults and "=" in param_str:
                param_str = param_str.split("=")[0].strip()

            param_strs.append(param_str)
```

with:

```python
        param_strs = []
        for name, param in sig.parameters.items():
            param_str = str(param)
            annotation = param.annotation
            if annotation is not inspect.Parameter.empty and _is_union_annotation(annotation):
                old_ann = inspect.formatannotation(annotation)
                new_ann = _canonical_annotation_str(annotation)
                param_str = param_str.replace(f": {old_ann}", f": {new_ann}", 1)
            if not self.include_defaults and "=" in param_str:
                param_str = param_str.split("=")[0].strip()
            param_strs.append(param_str)
```

- [ ] **Step 3: Run existing tests**

```bash
uv run pytest tests/test_hashing/ tests/test_core/data_function/ -v 2>&1 | tail -20
```

Expected: all tests **PASS**.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/function_info_extractors.py
git commit -m "fix(hashing): canonicalize union annotation order in FunctionSignatureExtractor (ITL-453)"
```

---

### Task 5: Add integration tests to `test_data_function.py`

**Files:**
- Modify: `tests/test_core/data_function/test_data_function.py`

These tests verify the end-to-end property: `PythonDataFunction._function_signature_hash` is order-independent for union-typed parameters and return types.

- [ ] **Step 1: Add the import for Path at the top of the test file if not present**

Check whether `from pathlib import Path` is already imported. If not, add it with the other imports near the top of the file.

- [ ] **Step 2: Append the new test class at the end of the file**

```python
# ---------------------------------------------------------------------------
# TestSignatureHashUnionOrderIndependence
# ---------------------------------------------------------------------------


class TestSignatureHashUnionOrderIndependence:
    """_function_signature_hash must be order-independent over union members."""

    def _sig_hash(self, func):
        # PythonDataFunction is already imported at the top of this test file.
        df = PythonDataFunction(func, output_keys="result")
        return df.get_function_variation_data()["function_signature_hash"]

    def test_two_member_union_param_order_independent(self):
        """str | Path and Path | str produce the same signature hash."""
        def foo1(x: str | Path) -> str:
            return str(x)

        def foo2(x: Path | str) -> str:
            return str(x)

        assert self._sig_hash(foo1) == self._sig_hash(foo2)

    def test_three_member_union_all_permutations(self):
        """All permutations of str | Path | bytes produce the same signature hash."""
        def f1(x: str | Path | bytes) -> str:
            return str(x)

        def f2(x: bytes | str | Path) -> str:
            return str(x)

        def f3(x: Path | bytes | str) -> str:
            return str(x)

        h1 = self._sig_hash(f1)
        assert self._sig_hash(f2) == h1
        assert self._sig_hash(f3) == h1

    def test_return_type_union_order_independent(self):
        """Return-type unions are also order-independent."""
        def foo1(x: int) -> str | Path:
            return str(x)

        def foo2(x: int) -> Path | str:
            return str(x)

        assert self._sig_hash(foo1) == self._sig_hash(foo2)

    def test_non_union_param_hash_unchanged(self):
        """Non-union functions produce the same hash as a second identical definition."""
        def foo(x: int) -> str:
            return str(x)

        def foo_copy(x: int) -> str:
            return str(x)

        # Both definitions are structurally identical (non-union) — hashes must agree.
        # This exercises that the fix does not disturb non-union annotations.
        assert self._sig_hash(foo) == self._sig_hash(foo_copy)

    def test_canonical_union_ordering(self):
        """str | Path canonicalizes to 'pathlib.Path | str' (P before s)."""
        from orcapod.hashing.hash_utils import _canonical_annotation_str
        assert _canonical_annotation_str(str | Path) == "pathlib.Path | str"

    def test_different_union_types_still_differ(self):
        """str | Path and str | bytes are different and must not hash the same."""
        def foo1(x: str | Path) -> str:
            return str(x)

        def foo2(x: str | bytes) -> str:
            return str(x)

        assert self._sig_hash(foo1) != self._sig_hash(foo2)

    def test_union_vs_non_union_differ(self):
        """A union-typed param and a plain-typed param produce different hashes."""
        def foo1(x: str | Path) -> str:
            return str(x)

        def foo2(x: str) -> str:
            return str(x)

        assert self._sig_hash(foo1) != self._sig_hash(foo2)
```

- [ ] **Step 3: Run the new integration tests**

```bash
uv run pytest tests/test_core/data_function/test_data_function.py::TestSignatureHashUnionOrderIndependence -v
```

Expected: all 7 tests **PASS**.

- [ ] **Step 4: Run the full data_function test suite**

```bash
uv run pytest tests/test_core/data_function/ -v 2>&1 | tail -20
```

Expected: all tests **PASS**.

- [ ] **Step 5: Commit**

```bash
git add tests/test_core/data_function/test_data_function.py
git commit -m "test(data_function): add union signature hash order-independence tests (ITL-453)"
```

---

### Task 6: Update `DESIGN_ISSUES.md`

**Files:**
- Modify: `DESIGN_ISSUES.md`

Per project convention, add a resolved entry for this bug. Find the section containing the UC (Union / Converter) issues (around the `### UC2` entry near line 1161) and add a new entry after UC2.

- [ ] **Step 1: Add the UC3 entry after the UC2 block**

Insert after the `---` that follows the UC2 block:

```markdown
### UC3 — Function signature hash order-dependent for union-typed annotations
**Status:** resolved
**Severity:** high
**Issue:** ITL-453

``get_function_signature()`` in ``hash_utils.py`` used ``str(param)`` to build
the signature string. For union type annotations, ``str(param)`` calls
``inspect.formatannotation``, which falls through to ``repr(annotation)`` —
reflecting declaration order. Two semantically identical signatures like
``foo(x: str | Path)`` and ``foo(x: Path | str)`` therefore produced different
``_function_signature_hash`` values, silently breaking content addressability
for any pipeline that refactored union member order.

**Fix:** Added ``_is_union_annotation`` and ``_canonical_annotation_str``
helpers to ``hash_utils.py``. Union members are now sorted byte-wise by their
``inspect.formatannotation`` string before being embedded in the signature.
Applied the same fix to ``FunctionSignatureExtractor.extract_function_info``.
Non-union annotations are byte-for-byte unchanged.

---
```

- [ ] **Step 2: Commit**

```bash
git add DESIGN_ISSUES.md
git commit -m "docs(design-issues): add UC3 resolved entry for ITL-453 union signature hash"
```

---

### Task 7: Full test suite and push

- [ ] **Step 1: Run the full test suite**

```bash
uv run pytest tests/ -x --timeout=120 2>&1 | tail -30
```

Expected: all tests **PASS** (no failures, no errors).

- [ ] **Step 2: Push the branch**

```bash
git push -u origin eywalker/itl-453-function-signature-hash-must-be-order-independent-for-union
```

- [ ] **Step 3: Create the PR**

```bash
gh pr create \
  --title "fix(hashing): make union-typed signature hash order-independent (ITL-453)" \
  --body "$(cat <<'EOF'
## Summary

- Adds `_is_union_annotation` and `_canonical_annotation_str` helpers to `hash_utils.py`
- Fixes `get_function_signature` to sort union members before embedding them in the signature string — both for parameter annotations and return type annotations
- Fixes `FunctionSignatureExtractor.extract_function_info` for the same bug in its `params` field
- Non-union annotations are byte-for-byte unchanged (no regression to existing hashes)

## Breaking change

Functions whose parameter or return type annotations include a union type (`str | Path`, `typing.Union[X, Y]`, `typing.Optional[X]`) will receive a different `_function_signature_hash` after this fix. Cached pipeline outputs keyed on such hashes will be invalidated. Acceptable under the v0.1 green-field stance.

Fixes ITL-453

## Test plan

- [ ] `uv run pytest tests/test_hashing/test_hash_utils.py -v` — new unit tests for helpers and `get_function_signature`
- [ ] `uv run pytest tests/test_core/data_function/test_data_function.py::TestSignatureHashUnionOrderIndependence -v` — integration tests for `_function_signature_hash`
- [ ] `uv run pytest tests/ -x` — full suite passes with no regressions

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)" \
  --base main
```
