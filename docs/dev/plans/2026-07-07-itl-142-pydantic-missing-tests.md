# ITL-142: Pydantic Missing Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two missing tests to `test_pydantic_logical_type_factory.py` covering `@computed_field` exclusion and validator behaviour on decode — the only genuine gaps between the current implementation and the ITL-142 success criteria.

**Architecture:** Both behaviours are already correct in `PydanticLogicalTypeFactory`; only test coverage is absent. The implementation walks `model_fields` (not `model_computed_fields`), so computed fields are naturally excluded. Reconstruction calls `Model(**kwargs)`, so Pydantic validators run automatically. We verify both by writing tests against the existing factory code.

**Tech Stack:** Python 3.12, Pydantic v2, PyArrow, pytest. Run tests with `uv run pytest`.

---

### Task 1: Add test for `@computed_field` exclusion

**Files:**
- Modify: `tests/test_extension_types/test_pydantic_logical_type_factory.py` (append after line 646)

- [ ] **Step 1: Add the module-level model with a computed field**

  First, update the existing pydantic import at the top of the file (line 11) to add `computed_field`:

  ```python
  from pydantic import BaseModel, PrivateAttr, computed_field
  ```

  Then append the following model definition to the module-level model block (after `_LiteralRoundTripModel`, around line 201). It must be at module scope so its FQCN is importable.

  ```python
  class _ModelWithComputedField(BaseModel):
      x: int
      y: int

      @computed_field
      @property
      def total(self) -> int:
          return self.x + self.y
  ```

- [ ] **Step 2: Write the failing test**

  Append after the last test in the file:

  ```python
  def test_computed_field_not_stored():
      """@computed_field properties must not appear in the Arrow struct schema.

      PydanticLogicalTypeFactory walks model_fields (not model_computed_fields),
      so @computed_field properties are naturally excluded. This test pins that
      behaviour so a future refactor cannot accidentally include them.
      """
      from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

      factory = PydanticLogicalTypeFactory()
      converter = _make_full_converter()
      lt = factory.create_for_python_type(_ModelWithComputedField, converter=converter)

      storage = lt.get_arrow_extension_type().storage_type
      field_names = {storage.field(i).name for i in range(storage.num_fields)}
      assert "x" in field_names
      assert "y" in field_names
      assert "total" not in field_names, (
          "@computed_field 'total' must not appear in the Arrow struct schema"
      )
      assert storage.num_fields == 2
  ```

- [ ] **Step 3: Run the test to verify it fails** (should PASS — but confirm it exists and runs)

  ```bash
  uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py::test_computed_field_not_stored -v
  ```

  Expected: `PASSED` — this confirms the behaviour is correct and the test is wired up.

---

### Task 2: Add test for validator behaviour on decode

**Files:**
- Modify: `tests/test_extension_types/test_pydantic_logical_type_factory.py`

- [ ] **Step 1: Add the module-level model with a validator**

  Update the existing pydantic import (already updated in Task 1) to also add `field_validator`:

  ```python
  from pydantic import BaseModel, PrivateAttr, computed_field, field_validator
  ```

  Then append this model after `_ModelWithComputedField`:

  ```python
  class _ModelWithValidator(BaseModel):
      name: str

      @field_validator("name")
      @classmethod
      def name_must_be_uppercase(cls, v: str) -> str:
          return v.upper()
  ```

- [ ] **Step 2: Write the failing test**

  Append after `test_computed_field_not_stored`:

  ```python
  def test_validator_runs_on_decode():
      """Pydantic validators must execute when a model is reconstructed from Arrow storage.

      Reconstruction calls Model(**kwargs), which runs Pydantic's validation pipeline
      automatically. This test uses a @field_validator that uppercases the 'name' field
      to provide a concrete, observable signal that validators are running.

      Note: non-idempotent validators (those that mutate values, like this one) mean
      the decoded instance may differ from a round-trip re-encoding of the original
      value — this is a Pydantic property, not an Orcapod bug.
      """
      from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

      converter = _make_full_converter()
      factory = PydanticLogicalTypeFactory()
      lt = factory.create_for_python_type(_ModelWithValidator, converter=converter)
      converter.register_logical_type(lt)

      # Store a lowercase value directly (bypassing validator on write for clarity)
      storage_value = {"name": "alice"}

      # Decode — validator must run and uppercase the name
      reconstructed = lt.storage_to_python(storage_value, converter)
      assert isinstance(reconstructed, _ModelWithValidator)
      assert reconstructed.name == "ALICE", (
          "Validator did not run on decode — expected 'alice' to be uppercased to 'ALICE'"
      )
  ```

- [ ] **Step 3: Run the new test**

  ```bash
  uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py::test_validator_runs_on_decode -v
  ```

  Expected: `PASSED`.

---

### Task 3: Run the full test file and commit

- [ ] **Step 1: Run the full pydantic test file**

  ```bash
  uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py -v
  ```

  Expected: `33 passed` (31 existing + 2 new).

- [ ] **Step 2: Commit**

  ```bash
  git add tests/test_extension_types/test_pydantic_logical_type_factory.py
  git commit -m "test(extension_types): add computed_field exclusion and validator-on-decode tests for PydanticLogicalTypeFactory (ITL-142)"
  ```

---

### Task 4: Create PR

- [ ] **Step 1: Push branch**

  ```bash
  git push -u origin eywalker/itl-142-add-pydantic-v2-support-to-orcapod-via-arrow-struct-encoding
  ```

- [ ] **Step 2: Create PR against main**

  ```bash
  gh pr create \
    --title "test(extension_types): add computed_field and validator tests for pydantic (ITL-142)" \
    --base main \
    --body "$(cat <<'EOF'
  ## Summary

  Closes ITL-142.

  Pydantic v2 `BaseModel` support was already fully implemented and automatically
  registered in the default context (`v0.1.json`) — this PR fills the last two test
  gaps identified during the ITL-142 verification pass:

  - **`test_computed_field_not_stored`**: pins that `@computed_field` properties are
    excluded from the Arrow struct schema (only `model_fields` is walked, not
    `model_computed_fields`).
  - **`test_validator_runs_on_decode`**: confirms that Pydantic validators run during
    reconstruction from Arrow storage (via `Model(**kwargs)`), and documents that
    non-idempotent validators may produce asymmetric round-trips.

  No production code changes — both behaviours were already correct.

  ## Test plan

  - [ ] `uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py -v` → 33 passed
  - [ ] `uv run pytest tests/test_extension_types/ tests/test_hashing/test_pydantic_dataclass_hashing.py -v` → all passed

  🤖 Generated with [Claude Code](https://claude.com/claude-code)
  EOF
  )"
  ```
