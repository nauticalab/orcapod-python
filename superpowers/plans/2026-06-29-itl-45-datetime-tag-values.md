# ITL-45: date/datetime Tag Values Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `datetime.date` and `datetime.datetime` as first-class valid types for Tag values, closing the gap between what the type system declares and what the Arrow converter can actually handle.

**Architecture:** Three independent layers are updated: (1) the `starfix` dependency is bumped to 0.4.0 so `pa.timestamp` columns can be content-hashed; (2) `UniversalTypeConverter` is extended with a direct `date → pa.date32()` mapping and a `pa.date32() → date` reverse mapping; (3) the public type aliases `TagValue` and `SupportedNativePythonData` are updated to formally include both types. Tests are written first (TDD) to nail down exact expected behavior before touching implementation.

**Tech Stack:** Python 3.12, PyArrow, starfix 0.4.0, pytest via `uv run pytest`

---

### Task 1: Bump starfix to 0.4.0

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Update the starfix pin in `pyproject.toml`**

  Find the line:
  ```
  "starfix~=0.3.0",
  ```
  Replace it with:
  ```
  "starfix~=0.4.0",
  ```

- [ ] **Step 2: Sync the environment**

  ```bash
  uv sync
  ```

  Expected: resolves and installs `starfix==0.4.x`. No errors.

- [ ] **Step 3: Smoke-test that timestamp hashing now works**

  ```bash
  uv run python -c "
  import pyarrow as pa
  from starfix import ArrowDigester
  from datetime import datetime, timezone
  dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
  t = pa.table({'ts': pa.array([dt], type=pa.timestamp('us', tz='UTC'))})
  h = ArrowDigester.hash_table(t)
  print('OK:', h.hex()[:16])
  "
  ```

  Expected: prints `OK:` followed by a hex string. No `NotImplementedError`.

- [ ] **Step 4: Commit**

  ```bash
  git add pyproject.toml uv.lock
  git commit -m "chore(deps): bump starfix to ~=0.4.0 for datetime hashing support"
  ```

---

### Task 2: Write failing tests for `date` type conversion

**Files:**
- Modify: `test-objective/unit/test_semantic_types.py`

- [ ] **Step 1: Add `date` imports at the top of the test file**

  The file already imports `pa` and `pytest`. Add `date` and `datetime` to the top-level imports:

  ```python
  from datetime import date, datetime, timezone
  ```

- [ ] **Step 2: Add a failing test for `python_type_to_arrow_type(date)`**

  Inside the existing `TestPythonToArrowType` class, add:

  ```python
  def test_date_to_date32(self, converter):
      result = converter.python_type_to_arrow_type(date)
      assert result == pa.date32()
  ```

- [ ] **Step 3: Add a failing test for `arrow_type_to_python_type(pa.date32())`**

  Inside the existing `TestArrowToPythonType` class, add:

  ```python
  def test_date32_to_date(self, converter):
      result = converter.arrow_type_to_python_type(pa.date32())
      assert result is date
  ```

- [ ] **Step 4: Add a failing test for schema round-trip with `date` and `datetime`**

  Inside the existing `TestSchemaConversionRoundtrip` class, add:

  ```python
  def test_date_datetime_schema_roundtrip(self, converter):
      from orcapod.types import Schema
      original = Schema({"dob": date, "ts": datetime})
      arrow_schema = converter.python_schema_to_arrow_schema(original)
      recovered = converter.arrow_schema_to_python_schema(arrow_schema)
      assert recovered["dob"] is date
      assert recovered["ts"] is datetime
  ```

- [ ] **Step 5: Run the new tests and verify they fail**

  ```bash
  uv run pytest test-objective/unit/test_semantic_types.py -k "date" -v
  ```

  Expected: `test_date32_to_date` and `test_date_datetime_schema_roundtrip` FAIL (currently `arrow_type_to_python_type(pa.date32())` returns `typing.Any`). `test_date_to_date32` may already pass via the `__name__` fallback — that's fine, it still documents required behaviour.

---

### Task 3: Implement `date` support in `UniversalTypeConverter`

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`

- [ ] **Step 1: Add `date` to the module-level import**

  Find the existing line near the top of the file:
  ```python
  from datetime import datetime, timezone
  ```
  Replace with:
  ```python
  from datetime import date, datetime, timezone
  ```

- [ ] **Step 2: Add `date → pa.date32()` to `_get_python_to_arrow_map()`**

  Inside `_get_python_to_arrow_map()`, find the existing `datetime` entry:
  ```python
  datetime: pa.timestamp("us", tz="UTC"),
  ```
  Add `date` immediately after it:
  ```python
  datetime: pa.timestamp("us", tz="UTC"),
  date: pa.date32(),
  ```

- [ ] **Step 3: Add `pa.date32() → date` to `_convert_arrow_to_python()`**

  Inside `_convert_arrow_to_python()`, find the existing timestamp branch:
  ```python
  elif pa.types.is_timestamp(arrow_type):
      return datetime
  ```
  Add the date branch **before** it:
  ```python
  elif pa.types.is_date(arrow_type):
      return date
  elif pa.types.is_timestamp(arrow_type):
      return datetime
  ```

- [ ] **Step 4: Run the tests and verify they all pass**

  ```bash
  uv run pytest test-objective/unit/test_semantic_types.py -k "date" -v
  ```

  Expected: all three new tests PASS. Also run the full semantic types suite to check for regressions:

  ```bash
  uv run pytest test-objective/unit/test_semantic_types.py -v
  ```

  Expected: all tests PASS.

- [ ] **Step 5: Commit**

  ```bash
  git add src/orcapod/semantic_types/universal_converter.py \
          test-objective/unit/test_semantic_types.py
  git commit -m "feat(types): add datetime.date <-> pa.date32() support in UniversalTypeConverter"
  ```

---

### Task 4: Update public type aliases

**Files:**
- Modify: `src/orcapod/types.py`

- [ ] **Step 1: Add `date` to the datetime import**

  Find the existing import in `types.py`:
  ```python
  from datetime import datetime
  ```
  Replace with:
  ```python
  from datetime import date, datetime
  ```

- [ ] **Step 2: Update `TagValue`**

  Find:
  ```python
  # TODO: accomodate other common data types such as datetime
  TagValue: TypeAlias = int | str | None | Collection["TagValue"]
  ```
  Replace with:
  ```python
  TagValue: TypeAlias = int | str | date | datetime | None | Collection["TagValue"]
  ```
  (Remove the TODO comment — it is now resolved.)

  Update its docstring to reflect the new types:
  ```python
  """A tag metadata value: an int, string, date, datetime, ``None``, or an
  arbitrarily nested collection thereof. Tags are used to label and organise
  data and datagrams."""
  ```

- [ ] **Step 3: Update `SupportedNativePythonData`**

  Find:
  ```python
  SupportedNativePythonData: TypeAlias = str | int | float | bool | bytes
  ```
  Replace with:
  ```python
  SupportedNativePythonData: TypeAlias = str | int | float | bool | bytes | date | datetime
  ```

  Update its docstring:
  ```python
  """The simple Python scalar types that have a direct Arrow / Polars
  correspondence, including temporal types ``date`` and ``datetime``."""
  ```

- [ ] **Step 4: Verify no import or type errors**

  ```bash
  uv run python -c "from orcapod.types import TagValue, SupportedNativePythonData; print(TagValue, SupportedNativePythonData)"
  ```

  Expected: prints the two type aliases without error.

- [ ] **Step 5: Commit**

  ```bash
  git add src/orcapod/types.py
  git commit -m "feat(types): add date and datetime to TagValue and SupportedNativePythonData"
  ```

---

### Task 5: Write and verify Tag integration tests

**Files:**
- Modify: `test-objective/unit/test_tag.py`

- [ ] **Step 1: Add imports at the top of `test_tag.py`**

  The file already imports `pa`, `pytest`, `Tag`, and `ColumnConfig`. Add:
  ```python
  from datetime import date, datetime, timezone
  ```

- [ ] **Step 2: Add the new test class**

  Append the following class to the end of `test_tag.py`:

  ```python
  # ---------------------------------------------------------------------------
  # date and datetime tag values
  # ---------------------------------------------------------------------------

  class TestTagDatetimeValues:
      """Tags accept date and datetime values and round-trip them correctly."""

      def test_date_tag_construction(self):
          ctx = _make_context()
          tag = Tag({"dob": date(2024, 1, 15)}, data_context=ctx)
          assert "dob" in tag.keys()

      def test_date_tag_as_dict_roundtrip(self):
          ctx = _make_context()
          d = date(2024, 1, 15)
          tag = Tag({"dob": d}, data_context=ctx)
          result = tag.as_dict()
          assert result["dob"] == d
          assert type(result["dob"]) is date

      def test_date_tag_as_table_schema(self):
          ctx = _make_context()
          tag = Tag({"dob": date(2024, 1, 15)}, data_context=ctx)
          table = tag.as_table()
          assert pa.types.is_date(table.schema.field("dob").type)

      def test_date_tag_content_hash(self):
          ctx = _make_context()
          tag = Tag({"dob": date(2024, 1, 15)}, data_context=ctx)
          h = tag.content_hash()
          assert h is not None

      def test_datetime_tag_construction(self):
          ctx = _make_context()
          dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
          tag = Tag({"ts": dt}, data_context=ctx)
          assert "ts" in tag.keys()

      def test_datetime_tag_as_dict_roundtrip(self):
          ctx = _make_context()
          dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
          tag = Tag({"ts": dt}, data_context=ctx)
          result = tag.as_dict()
          assert result["ts"] == dt
          assert type(result["ts"]) is datetime

      def test_datetime_tag_as_table_schema(self):
          ctx = _make_context()
          dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
          tag = Tag({"ts": dt}, data_context=ctx)
          table = tag.as_table()
          assert pa.types.is_timestamp(table.schema.field("ts").type)

      def test_datetime_tag_content_hash(self):
          ctx = _make_context()
          dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
          tag = Tag({"ts": dt}, data_context=ctx)
          h = tag.content_hash()
          assert h is not None

      def test_naive_datetime_raises(self):
          ctx = _make_context()
          with pytest.raises(ValueError, match="[Nn]aive datetime"):
              tag = Tag({"ts": datetime(2024, 1, 15)}, data_context=ctx)
              tag.as_table()  # conversion is lazy; trigger it

      def test_date_tag_schema_inference(self):
          """Schema inferred from an Arrow-backed Tag preserves date type."""
          ctx = _make_context()
          tag = Tag({"dob": date(2024, 1, 15)}, data_context=ctx)
          arrow_table = tag.as_table()
          tag2 = Tag(arrow_table, data_context=ctx)
          assert tag2.schema()["dob"] is date

      def test_datetime_tag_schema_inference(self):
          """Schema inferred from an Arrow-backed Tag preserves datetime type."""
          ctx = _make_context()
          dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
          tag = Tag({"ts": dt}, data_context=ctx)
          arrow_table = tag.as_table()
          tag2 = Tag(arrow_table, data_context=ctx)
          assert tag2.schema()["ts"] is datetime
  ```

- [ ] **Step 3: Run the new tests before implementing (verify current failures)**

  ```bash
  uv run pytest test-objective/unit/test_tag.py::TestTagDatetimeValues -v
  ```

  Expected failures before Task 3 is complete:
  - `test_date_tag_schema_inference` — `Any` instead of `date`
  - `test_datetime_tag_schema_inference` — `Any` instead of `datetime` (or may pass if ENG-387 landed)
  - `test_datetime_tag_content_hash` — `NotImplementedError` if starfix not yet bumped

  After Tasks 1–4 all complete, re-run:

  ```bash
  uv run pytest test-objective/unit/test_tag.py::TestTagDatetimeValues -v
  ```

  Expected: all 11 tests PASS.

- [ ] **Step 4: Run the full test-objective suite to check for regressions**

  ```bash
  uv run pytest test-objective/ -v
  ```

  Expected: all tests PASS.

- [ ] **Step 5: Run the main test suite**

  ```bash
  uv run pytest tests/ -v
  ```

  Expected: all tests PASS.

- [ ] **Step 6: Commit**

  ```bash
  git add test-objective/unit/test_tag.py
  git commit -m "test(tag): add date and datetime tag value tests"
  ```

---

### Task 6: Final verification and push

- [ ] **Step 1: Run the complete test suite one final time**

  ```bash
  uv run pytest test-objective/ tests/ -v
  ```

  Expected: all tests PASS, no warnings about `arrow_type_to_python_type` falling back to `Any`.

- [ ] **Step 2: Check out the feature branch**

  ```bash
  git checkout -b eywalker/itl-45-add-support-for-datetime-in-tag-values
  ```

- [ ] **Step 3: Push the branch**

  ```bash
  git push -u origin eywalker/itl-45-add-support-for-datetime-in-tag-values
  ```
