# ITL-639 Schema Hashing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `SchemaHandler` the explicit, canonical path for schema hashing by fixing the dispatch order in `hash_object` and implementing the handler — preserving all existing hash values with no cache bust.

**Architecture:** Two changes in the hashing layer: (1) move handler dispatch before `_is_structure` in `semantic_hasher.py` so `Schema` objects reach `SchemaHandler` instead of being intercepted as a plain `Mapping`; (2) replace the `NotImplementedError` stub in `SchemaHandler` with a real implementation that mirrors the accidental `_expand_mapping` path, adds Arrow-translatability validation, and receives `type_converter` at registration time.

**Tech Stack:** Python 3.10+, pytest, orcapod's `SemanticAwarePythonHasher` / `PythonTypeHandlerRegistry`, `TypeConverterProtocol`.

---

## Files Changed

| File | Action | What changes |
|------|--------|-------------|
| `src/orcapod/hashing/semantic_hashing/semantic_hasher.py` | Modify lines 172–190 | Swap order: handler dispatch moves before `_is_structure` check |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Modify lines 186–194, 529 | Replace `SchemaHandler` stub; wire `type_converter` at registration |
| `tests/test_hashing/test_type_annotation_golden.py` | Modify (append new class) | Add `TestSchemaHashBehavior` with 4 new tests |

---

## Task 1: Create the feature branch

- [ ] **Step 1: Create and check out the branch**

  ```bash
  cd /home/kurouto/kurouto-jobs/d96f41f0-a0ff-442f-8ab4-1981e391b07d/orcapod-python
  git checkout -b eywalker/itl-639-schema-hashing-must-go-through-arrow-representation-not
  git branch --show-current
  ```

  Expected output: `eywalker/itl-639-schema-hashing-must-go-through-arrow-representation-not`

---

## Task 2: Write the four new failing tests (TDD first)

**File:** `tests/test_hashing/test_type_annotation_golden.py`

- [ ] **Step 1: Append the new test class at the end of the file**

  Add the following at the very end of `tests/test_hashing/test_type_annotation_golden.py` (after line 274, after the `TestSchemaHashStability` class):

  ```python
  # ---------------------------------------------------------------------------
  # ITL-639 behaviour tests
  # ---------------------------------------------------------------------------


  class TestSchemaHashBehavior:
      """Explicit behavioural tests for SchemaHandler routing and validation.

      Verifies that after ITL-639:
      - Schema objects are dispatched to SchemaHandler (not _expand_mapping).
      - Schema hash is stable across module renames of orcapod logical types.
      - Unregistered types raise TypeError with a clear diagnostic message.
      - Schema.optional_fields is intentionally excluded from the hash.
      """

      def test_schema_routes_to_schema_handler(self, hasher, monkeypatch):
          """SchemaHandler.handle must be called when hashing a Schema."""
          from orcapod.hashing.semantic_hashing.builtin_handlers import SchemaHandler

          called = []
          original_handle = SchemaHandler.handle

          def patched(self, obj, h):
              called.append(obj)
              return original_handle(self, obj, h)

          monkeypatch.setattr(SchemaHandler, "handle", patched)
          hasher.hash_object(Schema({"x": int}))
          assert len(called) == 1, (
              "SchemaHandler.handle was not called — Schema is still being routed "
              "through _expand_mapping instead of the registered SchemaHandler."
          )

      def test_schema_hash_stable_across_module_rename(self, hasher):
          """Schema hash must not change when op.File.__module__ is mutated."""
          schema = Schema({"f": op.File})
          before = hasher.hash_object(schema).to_string()
          original_module = op.File.__module__
          try:
              op.File.__module__ = "orcapod.extension_types.file_type"  # simulate old path
              after = hasher.hash_object(schema).to_string()
          finally:
              op.File.__module__ = original_module
          assert before == after, (
              "Schema hash changed on module rename — TypeObjectHandler is not using "
              "logical_type_name for this type."
          )

      def test_schema_handler_rejects_unregistered_type(self, hasher):
          """Hashing a schema with an unregistered type must raise TypeError."""
          class _Unregistered:
              pass

          schema = Schema({"x": _Unregistered})
          with pytest.raises(TypeError, match="not Arrow-translatable"):
              hasher.hash_object(schema)

      def test_schema_hash_ignores_optional_fields(self, hasher):
          """Two schemas that differ only in optional_fields must hash identically."""
          s1 = Schema({"f": op.File})
          s2 = Schema({"f": op.File}, optional_fields={"f"})
          assert hasher.hash_object(s1).to_string() == hasher.hash_object(s2).to_string(), (
              "Schema hashes differ based on optional_fields — "
              "optionality must not be part of the schema hash."
          )
  ```

- [ ] **Step 2: Run the new tests to confirm the expected failures**

  ```bash
  cd /home/kurouto/kurouto-jobs/d96f41f0-a0ff-442f-8ab4-1981e391b07d/orcapod-python
  uv run pytest tests/test_hashing/test_type_annotation_golden.py::TestSchemaHashBehavior -v
  ```

  Expected result:
  - `test_schema_routes_to_schema_handler` → **FAIL** (`called` is empty — handler not reached)
  - `test_schema_hash_stable_across_module_rename` → **PASS** (ITL-638 already stable)
  - `test_schema_handler_rejects_unregistered_type` → **FAIL** (no TypeError raised, unregistered type falls through to `module.qualname`)
  - `test_schema_hash_ignores_optional_fields` → **PASS** (`_expand_mapping` already ignores `optional_fields`)

---

## Task 3: Fix dispatch order in `semantic_hasher.py`

**File:** `src/orcapod/hashing/semantic_hashing/semantic_hasher.py`, lines 172–190.

**What to change:** Move the handler dispatch block (currently lines 179–190) to run **before** the `_is_structure` check (currently lines 172–177). The block that starts `# Structures: expand into a tagged tree` must come **after** the handler dispatch.

- [ ] **Step 1: Replace the middle section of `hash_object`**

  Locate this exact block in `semantic_hasher.py` (lines 172–190):

  ```python
          # Structures: expand into a tagged tree, then hash the tree.
          if _is_structure(obj):
              expanded = self._expand_structure(
                  obj, _visited=frozenset(), resolver=resolver
              )
              return self._hash_to_content_hash(expanded)

          # Semantic hasher dispatch: handler returns a representative Python structure
          # (or a ContentHash as terminal); feed the result back into hash_object so
          # that returning a plain structure is equivalent to calling hash_object on it.
          handler = self._registry.get_handler(obj)
          if handler is not None:
              logger.debug(
                  "hash_object: dispatching %s to handler %s",
                  type(obj).__name__,
                  type(handler).__name__,
              )
              result = handler.handle(obj, self)
              return self.hash_object(result, resolver=resolver)
  ```

  Replace it with:

  ```python
          # Registered handler dispatch (BEFORE _is_structure — ensures SchemaHandler
          # takes priority over _expand_mapping for Schema objects, which are Mappings).
          handler = self._registry.get_handler(obj)
          if handler is not None:
              logger.debug(
                  "hash_object: dispatching %s to handler %s",
                  type(obj).__name__,
                  type(handler).__name__,
              )
              result = handler.handle(obj, self)
              return self.hash_object(result, resolver=resolver)

          # Structures: expand into a tagged tree, then hash the tree.
          if _is_structure(obj):
              expanded = self._expand_structure(
                  obj, _visited=frozenset(), resolver=resolver
              )
              return self._hash_to_content_hash(expanded)
  ```

- [ ] **Step 2: Run the two originally-failing tests to check progress**

  ```bash
  uv run pytest tests/test_hashing/test_type_annotation_golden.py::TestSchemaHashBehavior -v
  ```

  Expected result:
  - `test_schema_routes_to_schema_handler` → **FAIL** with `NotImplementedError` from `SchemaHandler` (dispatch fixed, handler reached — stub still raises)
  - `test_schema_handler_rejects_unregistered_type` → **FAIL** with `NotImplementedError` (same reason)
  - `test_schema_hash_stable_across_module_rename` → **FAIL** with `NotImplementedError` (now dispatches to SchemaHandler stub)
  - `test_schema_hash_ignores_optional_fields` → **FAIL** with `NotImplementedError` (same)

  All four failures with `NotImplementedError` confirms dispatch is now working — the stub is reached.

---

## Task 4: Implement `SchemaHandler` and wire `type_converter`

**File:** `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`

Two sub-changes: (a) replace the stub, (b) pass `type_converter` at registration.

- [ ] **Step 1: Replace the `SchemaHandler` stub (lines 186–194)**

  Locate this exact block:

  ```python
  class SchemaHandler:
      """Hasher for ``Schema`` objects."""

      def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
          if not isinstance(obj, Schema):
              raise TypeError(
                  f"SchemaHandler: expected a Schema, got {type(obj)!r}"
              )
          raise NotImplementedError("SchemaHandler is not yet implemented.")
  ```

  Replace it with:

  ```python
  class SchemaHandler:
      """Hasher for ``Schema`` objects.

      Canonical, explicit path for schema hashing. For each field, hashes the Python
      type via ``hasher.hash_object`` (which dispatches to ``TypeObjectHandler``, using
      the stable ``logical_type_name`` / Arrow extension name for registered types and the
      stable ``builtins.*`` / stdlib path for native types). Field names are sorted so the
      hash is deterministic regardless of insertion order.

      This produces the same hash as the previous accidental path through ``_expand_mapping``
      (Schema is a Mapping), preserving all existing hash values.

      ``Schema.optional_fields`` is intentionally excluded from the hash. Two schemas with
      the same field names and types but different optionality are hash-equivalent.
      Optionality is a Python-level execution contract (which parameters have defaults),
      not part of the structural identity used for caching or pipeline routing.

      When ``type_converter`` is provided, every field type is verified to be
      Arrow-translatable before hashing. A type that cannot be converted raises
      ``TypeError`` with a diagnostic message, catching unregistered types at hash time
      rather than silently falling back to a potentially unstable Python module path.

      Args:
          type_converter: Optional ``TypeConverterProtocol``. When provided, validates
              Arrow-translatability per field. When ``None`` (e.g. in tests without a
              full ``DataContext``), validation is skipped.
      """

      def __init__(self, type_converter: "TypeConverterProtocol | None" = None) -> None:
          self._type_converter = type_converter

      def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> Any:
          """Hash ``obj`` by hashing each field type individually and sorting by name.

          Args:
              obj: A ``Schema`` instance.
              hasher: The calling ``SemanticHasherProtocol`` — used to hash each field's
                  Python type.

          Returns:
              A ``dict[str, str]`` mapping field name to the ``to_string()`` of each
              field type's hash, sorted by field name for determinism.

          Raises:
              TypeError: If ``obj`` is not a ``Schema``, or if ``type_converter`` is
                  provided and a field type is not Arrow-translatable.
          """
          if not isinstance(obj, Schema):
              raise TypeError(
                  f"SchemaHandler: expected a Schema, got {type(obj)!r}"
              )
          result: dict[str, str] = {}
          for field_name, python_type in obj.items():
              if self._type_converter is not None:
                  try:
                      self._type_converter.python_type_to_arrow_type(python_type)
                  except (TypeError, ValueError) as exc:
                      raise TypeError(
                          f"SchemaHandler: field {field_name!r} has type "
                          f"{python_type!r} that is not Arrow-translatable. "
                          f"Every type in a schema must be Arrow-convertible — "
                          f"register it as an orcapod logical type or use a "
                          f"supported native type (int, str, float, bool, bytes, "
                          f"datetime, date)."
                      ) from exc
              result[field_name] = hasher.hash_object(python_type).to_string()
          # Sort by field name for determinism — matches _expand_mapping's sort_keys.
          return dict(sorted(result.items()))
  ```

- [ ] **Step 2: Wire `type_converter` into `SchemaHandler` at registration (line 529)**

  Locate this line in `register_builtin_python_type_handlers`:

  ```python
      registry.register(Schema, SchemaHandler())
  ```

  Replace it with:

  ```python
      registry.register(Schema, SchemaHandler(type_converter=type_converter))
  ```

- [ ] **Step 3: Run the new tests — all four must pass**

  ```bash
  uv run pytest tests/test_hashing/test_type_annotation_golden.py::TestSchemaHashBehavior -v
  ```

  Expected: all four tests **PASS**.

---

## Task 5: Verify no golden hash values changed

- [ ] **Step 1: Run the full test_hashing suite**

  ```bash
  uv run pytest tests/test_hashing/ -v
  ```

  Expected: every test **PASS**, including:
  - `TestGoldenStability::test_builtin_annotation_hashes_unchanged`
  - `TestGoldenStability::test_builtin_function_hashes_unchanged`
  - `TestGoldenCanonical::test_orcapod_annotation_hashes_changed`
  - `TestGoldenCanonical::test_orcapod_function_hashes_changed`
  - `TestSchemaHashStability::test_schema_hashes_stable` ← the 8 golden schema hashes must be byte-identical
  - `TestSchemaHashStability::test_all_golden_keys_covered`
  - `TestSchemaHashBehavior::test_schema_routes_to_schema_handler`
  - `TestSchemaHashBehavior::test_schema_hash_stable_across_module_rename`
  - `TestSchemaHashBehavior::test_schema_handler_rejects_unregistered_type`
  - `TestSchemaHashBehavior::test_schema_hash_ignores_optional_fields`

  If `TestSchemaHashStability::test_schema_hashes_stable` fails with mismatched hashes, the dispatch reorder or `SchemaHandler` implementation changed the hash structure. Check that `SchemaHandler.handle` returns `dict(sorted(result.items()))` where each value is `hasher.hash_object(python_type).to_string()` — this must match exactly what `_expand_mapping` produced before (a sorted dict of `{field_name: hash_token_string}`).

- [ ] **Step 2: Run the broader test suite to catch regressions**

  ```bash
  uv run pytest tests/ -x -q
  ```

  Expected: full suite passes. If any test fails, investigate before proceeding.

---

## Task 6: Update `DESIGN_ISSUES.md` if applicable

Per the CLAUDE.md convention: check `DESIGN_ISSUES.md` for any entry matching the dispatch order bug or SchemaHandler dead code.

- [ ] **Step 1: Check for a matching entry**

  ```bash
  grep -n -i "schema.*handler\|dispatch.*order\|_expand_mapping\|ITL-639" \
      /home/kurouto/kurouto-jobs/d96f41f0-a0ff-442f-8ab4-1981e391b07d/orcapod-python/DESIGN_ISSUES.md
  ```

  If a matching entry exists, update its status to `resolved` with a brief fix note. If none exists, no action needed (the issue was tracked in Linear).

---

## Task 7: Commit and open a PR

- [ ] **Step 1: Stage the three changed files**

  ```bash
  git add \
      src/orcapod/hashing/semantic_hashing/semantic_hasher.py \
      src/orcapod/hashing/semantic_hashing/builtin_handlers.py \
      tests/test_hashing/test_type_annotation_golden.py
  ```

- [ ] **Step 2: Commit**

  ```bash
  git commit -m "$(cat <<'EOF'
  fix(hashing): route Schema through SchemaHandler, not _expand_mapping

  Fixes the dispatch order in hash_object so SchemaHandler is reached for
  Schema objects (previously intercepted by _is_structure as a Mapping).
  Implements SchemaHandler with Arrow-translatability validation and wires
  type_converter at registration. Adds four regression tests. No hash values
  change — all golden assertions continue to pass. Closes ITL-639.

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
  EOF
  )"
  ```

- [ ] **Step 3: Push the branch**

  ```bash
  git push -u origin eywalker/itl-639-schema-hashing-must-go-through-arrow-representation-not
  ```

- [ ] **Step 4: Open the PR**

  ```bash
  gh pr create \
      --title "fix(hashing): route Schema through SchemaHandler, not _expand_mapping (ITL-639)" \
      --body "$(cat <<'EOF'
  ## Summary

  - Fixes dispatch order in `SemanticAwarePythonHasher.hash_object` so registered handlers run **before** the `_is_structure` check. This makes `SchemaHandler` reachable for `Schema` objects (previously intercepted as a plain `Mapping` by `_expand_mapping`).
  - Replaces the `NotImplementedError` stub in `SchemaHandler` with the canonical implementation: sorts fields by name, hashes each Python type via `TypeObjectHandler`, and validates Arrow-translatability when a `type_converter` is available.
  - Wires `type_converter` into `SchemaHandler` at registration so unregistered types are caught at hash time with a clear diagnostic error.
  - Adds four regression tests: dispatch routing, module-rename stability, Arrow-translatability guard, and `optional_fields` exclusion.
  - **No hash values change.** All eight golden schema hashes in `schema_hash_golden.json` remain byte-identical.

  Fixes ITL-639

  ## Test plan

  - [ ] `uv run pytest tests/test_hashing/ -v` — all tests pass, including `TestSchemaHashStability` with unchanged golden hashes
  - [ ] `uv run pytest tests/ -x -q` — full suite passes

  🤖 Generated with [Claude Code](https://claude.com/claude-code)
  EOF
  )"
  ```

---

## Self-Review Checklist

**Spec coverage:**
- ✅ Dispatch order fix (`semantic_hasher.py` Task 3)
- ✅ `SchemaHandler` implementation (Task 4, Step 1)
- ✅ `type_converter` wired at registration (Task 4, Step 2)
- ✅ Module-rename regression test (Task 2 → `test_schema_hash_stable_across_module_rename`)
- ✅ SchemaHandler-is-reached test (Task 2 → `test_schema_routes_to_schema_handler`)
- ✅ Arrow-translatability guard test (Task 2 → `test_schema_handler_rejects_unregistered_type`)
- ✅ `optional_fields` excluded test (Task 2 → `test_schema_hash_ignores_optional_fields`)
- ✅ Existing golden hash stability verified (Task 5)
- ✅ Convention doc: captured in `SchemaHandler` docstring and call-site comment

**Type consistency:** `SchemaHandler.handle` returns `dict[str, str]`, matching what `_expand_mapping` returned — no downstream type mismatch.

**No placeholders:** All steps contain complete, runnable code.
