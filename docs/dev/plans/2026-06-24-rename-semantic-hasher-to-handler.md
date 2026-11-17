# Rename *SemanticHasher → *Handler, PythonTypeSemanticHasherRegistry → PythonTypeHandlerRegistry

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mechanically rename all `*SemanticHasher` handler classes to `*Handler`, all `PythonTypeSemanticHasherRegistry` variants to `PythonTypeHandlerRegistry`, and the `type_semantic_hasher_registry` param/property to `type_handler_registry` — no logic changes.

**Architecture:** Pure find-and-replace of identifiers across ~10 source files and 2 JSON configs. Every old name maps 1-to-1 to a new name. No logic, no interface changes, no backward-compat shims (greenfield project).

**Tech Stack:** Python, JSON, uv/pytest

---

## File Map

| File | What changes |
|---|---|
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | 11 class names + function name + docstring/string literals |
| `src/orcapod/hashing/semantic_hashing/type_handler_registry.py` | 3 class/function names + docstrings + internal log strings |
| `src/orcapod/hashing/semantic_hashing/semantic_hasher.py` | param + property name `type_semantic_hasher_registry` → `type_handler_registry` + docstring |
| `src/orcapod/hashing/semantic_hashing/__init__.py` | imports + `__all__` |
| `src/orcapod/hashing/__init__.py` | imports + `__all__` |
| `src/orcapod/hashing/defaults.py` | function name + import + docstring |
| `src/orcapod/hashing/versioned_hashers.py` | param name + import |
| `src/orcapod/protocols/hashing_protocols.py` | property name in `SemanticHasherProtocol` + TYPE_CHECKING import |
| `src/orcapod/contexts/data/v0.1.json` | top-level key, `_class` values, `_ref` value, sub-key |
| `src/orcapod/contexts/data/schemas/context_schema.json` | property key |
| `tests/test_hashing/test_semantic_hasher.py` | imports + usage |
| `tests/test_hashing/test_uuid_handler.py` | imports + usage |
| `tests/test_hashing/test_extension_type_hashing.py` | no old names (already clean) |
| `test-objective/unit/test_hashing.py` | imports, class names, type annotations, comments |

---

## Rename Reference Table

### Handler classes (builtin_handlers.py + all callers)

| Old | New |
|---|---|
| `PathSemanticHasher` | `PathHandler` |
| `UPathSemanticHasher` | `UPathHandler` |
| `UUIDSemanticHasher` | `UUIDHandler` |
| `BytesSemanticHasher` | `BytesHandler` |
| `FunctionSemanticHasher` | `FunctionHandler` |
| `TypeObjectSemanticHasher` | `TypeObjectHandler` |
| `SpecialFormSemanticHasher` | `SpecialFormHandler` |
| `GenericAliasSemanticHasher` | `GenericAliasHandler` |
| `UnionTypeSemanticHasher` | `UnionTypeHandler` |
| `ArrowTableSemanticHasher` | `ArrowTableHandler` |
| `SchemaSemanticHasher` | `SchemaHandler` |
| `register_builtin_python_type_semantic_hashers` | `register_builtin_python_type_handlers` |

### Registry classes (type_handler_registry.py + all callers)

| Old | New |
|---|---|
| `PythonTypeSemanticHasherRegistry` | `PythonTypeHandlerRegistry` |
| `BuiltinPythonTypeSemanticHasherRegistry` | `BuiltinPythonTypeHandlerRegistry` |
| `get_default_python_type_semantic_hasher_registry` | `get_default_python_type_handler_registry` |

### Parameter/property (semantic_hasher.py + all callers)

| Old | New |
|---|---|
| `type_semantic_hasher_registry` | `type_handler_registry` |

---

## Task 1: Rename class definitions and internal strings in `builtin_handlers.py`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`

- [ ] **Step 1: Apply all renames in builtin_handlers.py**

  Changes needed (all are identifier or string-literal renames only):
  - Module docstring: update all `*SemanticHasher` names and `register_builtin_python_type_semantic_hashers`
  - TYPE_CHECKING import: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - All 11 class definitions: `class PathSemanticHasher` → `class PathHandler`, etc.
  - Error messages inside class bodies: e.g. `"PathSemanticHasher: path does not exist"` → `"PathHandler: path does not exist"`
  - `logger.debug` strings: e.g. `"PathSemanticHasher: hashing file content"` → `"PathHandler: hashing file content"`
  - Function `register_builtin_python_type_semantic_hashers` → `register_builtin_python_type_handlers`
  - Docstring inside that function: update `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - Final `logger.debug` string: `"register_builtin_python_type_semantic_hashers: registered %d hashers"` → `"register_builtin_python_type_handlers: registered %d hashers"`

- [ ] **Step 2: Verify file parses correctly**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "from orcapod.hashing.semantic_hashing import builtin_handlers; print('OK')"
  ```
  Expected: `OK`

---

## Task 2: Rename class definitions in `type_handler_registry.py`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/type_handler_registry.py`

- [ ] **Step 1: Apply all renames in type_handler_registry.py**

  Changes needed:
  - Module docstring: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - Class `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - `__repr__` method: `"PythonTypeSemanticHasherRegistry(registered=..."` → `"PythonTypeHandlerRegistry(registered=..."`
  - `logger.debug` strings that mention `PythonTypeSemanticHasherRegistry`
  - Function `get_default_python_type_semantic_hasher_registry` → `get_default_python_type_handler_registry`
  - The function body's import: `get_default_python_type_semantic_hasher_registry as _get` → `get_default_python_type_handler_registry as _get`
  - Class `BuiltinPythonTypeSemanticHasherRegistry` → `BuiltinPythonTypeHandlerRegistry`
  - Docstring: `"A PythonTypeSemanticHasherRegistry pre-populated..."` → `"A PythonTypeHandlerRegistry pre-populated..."`
  - `super().__init__()` call — no change needed
  - Import inside `__init__`: `register_builtin_python_type_semantic_hashers` → `register_builtin_python_type_handlers`
  - Call: `register_builtin_python_type_semantic_hashers(self, ...)` → `register_builtin_python_type_handlers(self, ...)`

- [ ] **Step 2: Verify file parses correctly**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry; print('OK')"
  ```
  Expected: `OK`

---

## Task 3: Rename param/property in `semantic_hasher.py`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/semantic_hasher.py`

- [ ] **Step 1: Apply renames in semantic_hasher.py**

  Changes needed:
  - Import: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - Docstring parameter: `type_semantic_hasher_registry:` → `type_handler_registry:`
  - Constructor param: `type_semantic_hasher_registry: PythonTypeHandlerRegistry | None = None` → `type_handler_registry: PythonTypeHandlerRegistry | None = None`
  - Constructor body: `if type_semantic_hasher_registry is None:` → `if type_handler_registry is None:`
  - Constructor body: `from orcapod.hashing.defaults import get_default_python_type_semantic_hasher_registry` → `get_default_python_type_handler_registry`
  - Constructor body: `self._registry = get_default_python_type_semantic_hasher_registry()` → `get_default_python_type_handler_registry()`
  - Constructor body: `else: self._registry = type_semantic_hasher_registry` → `else: self._registry = type_handler_registry`
  - Property `type_semantic_hasher_registry` → `type_handler_registry`
  - Property docstring: `"Return the ``PythonTypeSemanticHasherRegistry``..."` → `"Return the ``PythonTypeHandlerRegistry``..."`
  - Property return type annotation: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - Error message in `_handle_unknown`: `"via the PythonTypeSemanticHasherRegistry or"` → `"via the PythonTypeHandlerRegistry or"`

- [ ] **Step 2: Verify file parses correctly**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher; print('OK')"
  ```
  Expected: `OK`

---

## Task 4: Update `semantic_hashing/__init__.py`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/__init__.py`

- [ ] **Step 1: Apply renames**

  Changes needed:
  - Module docstring: all `*SemanticHasher` names → `*Handler` equivalents
  - Import from `builtin_handlers`: `BytesSemanticHasher` → `BytesHandler`, etc.; `register_builtin_python_type_semantic_hashers` → `register_builtin_python_type_handlers`
  - Import from `type_handler_registry`: `BuiltinPythonTypeSemanticHasherRegistry` → `BuiltinPythonTypeHandlerRegistry`, `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - `__all__`: update all entries to new names

- [ ] **Step 2: Verify**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "from orcapod.hashing.semantic_hashing import PathHandler, PythonTypeHandlerRegistry, register_builtin_python_type_handlers; print('OK')"
  ```
  Expected: `OK`

---

## Task 5: Update `hashing/__init__.py`

**Files:**
- Modify: `src/orcapod/hashing/__init__.py`

- [ ] **Step 1: Apply renames**

  Changes needed:
  - Module docstring: update all old names
  - Import from `defaults`: `get_default_python_type_semantic_hasher_registry` → `get_default_python_type_handler_registry`
  - Import from `builtin_handlers`: `BytesSemanticHasher` → `BytesHandler`, etc.; `register_builtin_python_type_semantic_hashers` → `register_builtin_python_type_handlers`
  - Import from `type_handler_registry`: `BuiltinPythonTypeSemanticHasherRegistry` → `BuiltinPythonTypeHandlerRegistry`, `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - `__all__`: update all entries to new names

- [ ] **Step 2: Verify**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "from orcapod.hashing import PythonTypeHandlerRegistry, get_default_python_type_handler_registry, BytesHandler; print('OK')"
  ```
  Expected: `OK`

---

## Task 6: Update `hashing/defaults.py`

**Files:**
- Modify: `src/orcapod/hashing/defaults.py`

- [ ] **Step 1: Apply renames**

  Changes needed:
  - Import: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - Function name: `get_default_python_type_semantic_hasher_registry` → `get_default_python_type_handler_registry`
  - Return type annotation: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - Docstring: update class name references
  - Function body: `get_default_context().semantic_hasher.type_semantic_hasher_registry` → `get_default_context().semantic_hasher.type_handler_registry`

- [ ] **Step 2: Verify**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "from orcapod.hashing.defaults import get_default_python_type_handler_registry; print('OK')"
  ```
  Expected: `OK`

---

## Task 7: Update `hashing/versioned_hashers.py`

**Files:**
- Modify: `src/orcapod/hashing/versioned_hashers.py`

- [ ] **Step 1: Apply renames**

  Changes needed:
  - Function param: `type_semantic_hasher_registry: "Any | None" = None` → `type_handler_registry: "Any | None" = None`
  - Docstring param description: `type_semantic_hasher_registry:` → `type_handler_registry:`
  - Import inside function: `get_default_python_type_semantic_hasher_registry` → `get_default_python_type_handler_registry`
  - Variable: `type_semantic_hasher_registry = get_default_python_type_semantic_hasher_registry()` → `type_handler_registry = get_default_python_type_handler_registry()`
  - `SemanticAwarePythonHasher(... type_semantic_hasher_registry=type_semantic_hasher_registry ...)` → `... type_handler_registry=type_handler_registry ...`

- [ ] **Step 2: Verify**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "from orcapod.hashing.versioned_hashers import get_versioned_semantic_hasher; print('OK')"
  ```
  Expected: `OK`

---

## Task 8: Update `protocols/hashing_protocols.py`

**Files:**
- Modify: `src/orcapod/protocols/hashing_protocols.py`

- [ ] **Step 1: Apply renames**

  Changes needed:
  - TYPE_CHECKING import: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - `SemanticHasherProtocol.type_semantic_hasher_registry` property → `type_handler_registry`
  - Property docstring: `"Return the PythonTypeSemanticHasherRegistry..."` → `"Return the PythonTypeHandlerRegistry..."`
  - Property return type annotation: `"PythonTypeSemanticHasherRegistry"` → `"PythonTypeHandlerRegistry"`

- [ ] **Step 2: Verify**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "from orcapod.protocols.hashing_protocols import SemanticHasherProtocol; print('OK')"
  ```
  Expected: `OK`

---

## Task 9: Update `contexts/data/v0.1.json`

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 1: Apply renames**

  Changes needed (4 renames):
  1. Top-level key `"python_type_semantic_hasher_registry"` → `"python_type_handler_registry"`
  2. All `"_class"` values with `*SemanticHasher` suffix — e.g.:
     - `"...builtin_handlers.BytesSemanticHasher"` → `"...builtin_handlers.BytesHandler"`
     - `"...builtin_handlers.PathSemanticHasher"` → `"...builtin_handlers.PathHandler"`
     - `"...builtin_handlers.UPathSemanticHasher"` → `"...builtin_handlers.UPathHandler"`
     - `"...builtin_handlers.UUIDSemanticHasher"` → `"...builtin_handlers.UUIDHandler"`
     - `"...builtin_handlers.FunctionSemanticHasher"` → `"...builtin_handlers.FunctionHandler"`
     - `"...builtin_handlers.TypeObjectSemanticHasher"` → `"...builtin_handlers.TypeObjectHandler"`
     - `"...builtin_handlers.GenericAliasSemanticHasher"` → `"...builtin_handlers.GenericAliasHandler"`
     - `"...builtin_handlers.UnionTypeSemanticHasher"` → `"...builtin_handlers.UnionTypeHandler"`
     - `"...builtin_handlers.SpecialFormSemanticHasher"` → `"...builtin_handlers.SpecialFormHandler"`
     - `"...builtin_handlers.ArrowTableSemanticHasher"` → `"...builtin_handlers.ArrowTableHandler"`
     - `"...type_handler_registry.PythonTypeSemanticHasherRegistry"` → `"...type_handler_registry.PythonTypeHandlerRegistry"`
  3. Inside `semantic_hasher._config`: sub-key `"type_semantic_hasher_registry"` → `"type_handler_registry"`
  4. Inside `semantic_hasher._config.type_handler_registry`: `"_ref": "python_type_semantic_hasher_registry"` → `"_ref": "python_type_handler_registry"`

- [ ] **Step 2: Verify JSON is valid and context loads**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "import json; json.load(open('src/orcapod/contexts/data/v0.1.json')); print('JSON OK')"
  uv run python -c "from orcapod.contexts import get_default_context; ctx = get_default_context(); print('Context OK')"
  ```
  Expected: `JSON OK` then `Context OK`

---

## Task 10: Update `contexts/data/schemas/context_schema.json`

**Files:**
- Modify: `src/orcapod/contexts/data/schemas/context_schema.json`

- [ ] **Step 1: Apply renames**

  Changes needed:
  - Property key `"python_type_semantic_hasher_registry"` → `"python_type_handler_registry"` (in `properties` section)
  - Description string within that property: `"ObjectSpec for the PythonTypeSemanticHasherRegistry..."` → `"ObjectSpec for the PythonTypeHandlerRegistry..."`
  - In the `examples` section: `"type_semantic_hasher_registry"` sub-key → `"type_handler_registry"`, and `"_ref": "python_type_semantic_hasher_registry"` → `"_ref": "python_type_handler_registry"`

- [ ] **Step 2: Verify JSON is valid**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -c "import json; json.load(open('src/orcapod/contexts/data/schemas/context_schema.json')); print('Schema JSON OK')"
  ```
  Expected: `Schema JSON OK`

---

## Task 11: Update test files

**Files:**
- Modify: `tests/test_hashing/test_semantic_hasher.py`
- Modify: `tests/test_hashing/test_uuid_handler.py`
- Modify: `test-objective/unit/test_hashing.py`

- [ ] **Step 1: Update `tests/test_hashing/test_semantic_hasher.py`**

  Changes needed:
  - Import: `register_builtin_python_type_semantic_hashers` → `register_builtin_python_type_handlers`
  - Import: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - Import: `get_default_python_type_semantic_hasher_registry` → `get_default_python_type_handler_registry`
  - `make_hasher` body: `registry = PythonTypeSemanticHasherRegistry()` → `PythonTypeHandlerRegistry()`, `register_builtin_python_type_semantic_hashers(registry)` → `register_builtin_python_type_handlers(registry)`, `type_semantic_hasher_registry=registry` → `type_handler_registry=registry`
  - All other usages of these names throughout the file (type annotations, variable names, docstrings, comments)

- [ ] **Step 2: Update `tests/test_hashing/test_uuid_handler.py`**

  Changes needed:
  - Import: `register_builtin_python_type_semantic_hashers` → `register_builtin_python_type_handlers`
  - Import: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - `_make_hasher` body: same pattern as above
  - `type_semantic_hasher_registry=registry` → `type_handler_registry=registry`

- [ ] **Step 3: Update `test-objective/unit/test_hashing.py`**

  Changes needed (this file has many occurrences — all follow the same pattern):
  - Imports: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`, `BuiltinPythonTypeSemanticHasherRegistry` → `BuiltinPythonTypeHandlerRegistry`
  - All fixture/function type annotations: `PythonTypeSemanticHasherRegistry` → `PythonTypeHandlerRegistry`
  - All constructor calls: `type_semantic_hasher_registry=registry` → `type_handler_registry=registry`
  - All class names in test bodies: `PythonTypeSemanticHasherRegistry()` → `PythonTypeHandlerRegistry()`
  - All `BuiltinPythonTypeSemanticHasherRegistry()` → `BuiltinPythonTypeHandlerRegistry()`
  - All comments/docstrings mentioning old names

- [ ] **Step 4: Verify test files parse**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run python -m py_compile tests/test_hashing/test_semantic_hasher.py && echo "OK"
  uv run python -m py_compile tests/test_hashing/test_uuid_handler.py && echo "OK"
  uv run python -m py_compile test-objective/unit/test_hashing.py && echo "OK"
  ```
  Expected: three `OK` lines

---

## Task 12: Run tests and commit

- [ ] **Step 1: Run hashing tests**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run pytest tests/test_hashing/ -x -q
  ```
  Expected: all tests pass

- [ ] **Step 2: Run full test suite (excluding deleted semantic types)**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  uv run pytest tests/ -x -q --ignore=tests/test_semantic_types
  ```
  Expected: all tests pass

- [ ] **Step 3: Confirm no remaining old names in source**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  grep -rn "PathSemanticHasher\|UPathSemanticHasher\|UUIDSemanticHasher\|BytesSemanticHasher\|FunctionSemanticHasher\|TypeObjectSemanticHasher\|SpecialFormSemanticHasher\|GenericAliasSemanticHasher\|UnionTypeSemanticHasher\|ArrowTableSemanticHasher\|SchemaSemanticHasher\|PythonTypeSemanticHasherRegistry\|BuiltinPythonTypeSemanticHasherRegistry\|get_default_python_type_semantic_hasher_registry\|register_builtin_python_type_semantic_hashers\|type_semantic_hasher_registry" src/ tests/ test-objective/ --include="*.py" --include="*.json" | grep -v "^Binary"
  ```
  Expected: no matches (zero lines)

- [ ] **Step 4: Commit**

  ```bash
  cd /home/kurouto/kurouto-jobs/dc15d84f-7281-48b5-9e17-435e9a04f175/orcapod-python
  git add src/orcapod/hashing/semantic_hashing/builtin_handlers.py
  git add src/orcapod/hashing/semantic_hashing/type_handler_registry.py
  git add src/orcapod/hashing/semantic_hashing/semantic_hasher.py
  git add src/orcapod/hashing/semantic_hashing/__init__.py
  git add src/orcapod/hashing/__init__.py
  git add src/orcapod/hashing/defaults.py
  git add src/orcapod/hashing/versioned_hashers.py
  git add src/orcapod/protocols/hashing_protocols.py
  git add src/orcapod/contexts/data/v0.1.json
  git add src/orcapod/contexts/data/schemas/context_schema.json
  git add tests/test_hashing/test_semantic_hasher.py
  git add tests/test_hashing/test_uuid_handler.py
  git add test-objective/unit/test_hashing.py
  git add superpowers/plans/2026-06-24-rename-semantic-hasher-to-handler.md
  git commit -m "refactor(hashing): rename *SemanticHasher → *Handler, PythonTypeSemanticHasherRegistry → PythonTypeHandlerRegistry"
  ```
